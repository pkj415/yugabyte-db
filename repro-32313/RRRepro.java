// RRRepro — standalone reproducer for the COLOCATED-table REPEATABLE READ snapshot violation on
// YugabyteDB master 2.31. A read-only RR transaction reads the full table twice (two textually
// different but semantically identical SELECTs); at REPEATABLE READ the two reads MUST be identical.
// Under concurrent jsonb transfers on a COLOCATED table, the second read can observe a committed
// write the first did not -> the txn commits with two different reads (NOT a retryable read-restart).
//
// Uses the EXACT schema the fuzzer reproduced on (dumped from the harness): bigint `id desc` range
// PK, jsonb `bal`, several extra columns with volatile defaults, and an EXPRESSION INDEX on the
// extracted jsonb value `((bal->'a'->>'b')::bigint)` (the column the reader projects). Default plans;
// only yb_enable_bitmapscan=off (the harness default). Run vs a colocated DB on master:
//   java -cp jdbc-yugabytedb.jar:. RRRepro "jdbc:yugabytedb://127.0.0.1:6433/coloyb?load-balance=false" 30
import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.concurrent.atomic.*;

public class RRRepro {

    static final int N = Integer.parseInt(
        System.getenv().getOrDefault("RR_ROWS", "16")
    );
    static final int CLIENTS = Integer.parseInt(
        System.getenv().getOrDefault("RR_CLIENTS", "12")
    );
    static volatile boolean stop = false;
    static final AtomicLong violations = new AtomicLong(),
        reads = new AtomicLong(),
        restarts = new AtomicLong();
    static String URL;
    static volatile String example = null;

    // Timestamps in the same format as the postgres log, so a violation line can
    // be grepped straight against postgresql-*.log / the tserver INFO log.
    static final DateTimeFormatter TSFMT = DateTimeFormatter.ofPattern(
        "yyyy-MM-dd HH:mm:ss.SSS"
    );

    static String now() {
        return LocalDateTime.now().format(TSFMT);
    }

    static int backendPid(Connection conn) throws SQLException {
        try (
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select pg_backend_pid()")
        ) {
            rs.next();
            return rs.getInt(1);
        }
    }

    static Connection c() throws SQLException {
        return DriverManager.getConnection(URL, "yugabyte", "yugabyte");
    }

    // EXACT harness schema for the reproducing config (dumped via DUMP_DDL)
    static void setup() throws SQLException {
        boolean noColo = "1".equals(System.getenv("RR_NO_COLO")); // control: non-colocated
        boolean noExpr = "1".equals(System.getenv("RR_NO_EXPR_IDX")); // bisect: drop the expr index
        try (Connection a = c(); Statement s = a.createStatement()) {
            a.setAutoCommit(true);
            try {
                s.execute("drop table if exists rr_acct cascade");
            } catch (Exception e) {}
            s.execute(
                "create table rr_acct (id bigint, bal jsonb, region int, " +
                    "x0 macaddr default '08:00:2b:01:02:03'::macaddr, " +
                    "x1 char(8) default lpad((random()*9999999)::int::text,8,'0'), " +
                    "x2 text[] default array[md5(random()::text)], " +
                    "x3 uuid default gen_random_uuid(), " +
                    "x4 numeric[] default array[(random()*100)::numeric], " +
                    "primary key (id desc))" +
                    (noColo ? "" : " with (colocation = true)")
            );
            if (!noExpr) s.execute(
                "create index rr_acct_x on rr_acct (((bal->'a'->>'b')::bigint))"
            );
            s.execute("create index rr_acct_x1 on rr_acct (x1 asc)");
            s.execute("create index rr_acct_x3 on rr_acct (x3 asc)");
            s.execute(
                "insert into rr_acct(id,bal,region) select g, " +
                    "jsonb_build_object('a',jsonb_build_object('b',1000)), g%3 " +
                    "from generate_series(0," +
                    (N - 1) +
                    ") g"
            );
            System.out.printf(
                "setup: colocated=%s expr_index=%s%n",
                !noColo,
                !noExpr
            );
        }
    }

    static final String[] XFER = {
        "update rr_acct set bal=jsonb_build_object('a',jsonb_build_object('b',(bal->'a'->>'b')::bigint + (%d))) where id=%d",
        "update rr_acct set bal=jsonb_build_object('a',jsonb_build_object('b',(bal->'a'->>'b')::bigint + (case when true then (%d) else 0 end))) where id=%d",
        "with d as (select (%d)::bigint as x) update rr_acct set bal=jsonb_build_object('a',jsonb_build_object('b',(bal->'a'->>'b')::bigint + (select x from d))) where id=%d",
    };
    static final String R1 =
        "select * from (select id, (bal->'a'->>'b')::bigint as bal from rr_acct where (true)) u order by id asc";
    static final String R2 =
        "select * from (select id, (bal->'a'->>'b')::bigint as bal from rr_acct where 1=1) u order by u.id";

    static List<long[]> read(Statement s, String q) throws SQLException {
        List<long[]> out = new ArrayList<>();
        try (ResultSet rs = s.executeQuery(q)) {
            while (rs.next())
                out.add(new long[] { rs.getLong(1), rs.getLong(2) });
        }
        return out;
    }

    static void client(int idx) {
        Random rng = new Random(idx + 1);
        try (Connection conn = c()) {
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(
                Connection.TRANSACTION_REPEATABLE_READ
            );
            try (Statement s = conn.createStatement()) {
                s.execute("set yb_enable_bitmapscan=off");
                conn.commit();
            }
            final int pid = backendPid(conn);
            conn.commit();
            while (!stop) {
                try {
                    if (rng.nextInt(100) < 18) {
                        // RRSNAP double-read txn
                        try (Statement s = conn.createStatement()) {
                            String t0 = now();
                            List<long[]> a = read(s, R1);
                            String t1 = now();
                            if (rng.nextBoolean()) s.executeQuery(
                                "select 1"
                            ).close(); // optional noise (as rendered)
                            String t2 = now();
                            List<long[]> b = read(s, R2);
                            String t3 = now();
                            conn.commit();
                            String t4 = now();
                            reads.incrementAndGet();
                            if (!eq(a, b)) {
                                violations.incrementAndGet();
                                String d = diff(a, b);
                                if (example == null) example = d;
                                synchronized (RRRepro.class) {
                                    System.out.printf(
                                        "%n*** VIOLATION backend_pid=%d%n" +
                                        "    R1  start=%s end=%s%n" +
                                        "    R2  start=%s end=%s%n" +
                                        "    COMMIT at %s%n" +
                                        "    diff: %s%n",
                                        pid, t0, t1, t2, t3, t4, d
                                    );
                                    System.out.flush();
                                }
                            }
                        }
                    } else {
                        // atomic jsonb transfer (random shape)
                        int i = rng.nextInt(N),
                            j = rng.nextInt(N);
                        if (i == j) continue;
                        try (Statement s = conn.createStatement()) {
                            String f = XFER[rng.nextInt(XFER.length)];
                            s.execute(String.format(f, -25, i));
                            s.execute(String.format(f, 25, j));
                            conn.commit();
                        }
                    }
                } catch (SQLException e) {
                    restarts.incrementAndGet();
                    try {
                        conn.rollback();
                    } catch (SQLException x) {}
                }
            }
        } catch (SQLException e) {
            System.err.println("client " + idx + ": " + e.getMessage());
        }
    }

    static boolean eq(List<long[]> a, List<long[]> b) {
        if (a.size() != b.size()) return false;
        for (int k = 0; k < a.size(); k++) if (
            a.get(k)[0] != b.get(k)[0] || a.get(k)[1] != b.get(k)[1]
        ) return false;
        return true;
    }

    static String diff(List<long[]> a, List<long[]> b) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < Math.min(a.size(), b.size()); k++) if (
            a.get(k)[1] != b.get(k)[1]
        ) sb.append("id=")
            .append(a.get(k)[0])
            .append(" r1=")
            .append(a.get(k)[1])
            .append(" r2=")
            .append(b.get(k)[1])
            .append("; ");
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        URL = args[0];
        int dur = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        Class.forName("com.yugabyte.Driver");
        setup();
        Thread[] ts = new Thread[CLIENTS];
        for (int i = 0; i < CLIENTS; i++) {
            final int idx = i;
            ts[i] = new Thread(() -> client(idx));
            ts[i].start();
        }
        Thread.sleep(dur * 1000L);
        stop = true;
        for (Thread t : ts) t.join();
        System.out.printf(
            "RR double-reads=%d  SILENT_VIOLATIONS=%d  retries/restarts=%d%n",
            reads.get(),
            violations.get(),
            restarts.get()
        );
        System.out.println("example: " + example);
        System.out.println(
            violations.get() > 0
                ? "*** REPRODUCED: REPEATABLE READ snapshot violation on colocated table ***"
                : "no violation in this run"
        );
    }
}
