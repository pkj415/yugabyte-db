// Copyright (c) YugaByte, Inc.

import React, { Component, PropTypes } from 'react';
import { BootstrapTable, TableHeaderColumn } from 'react-bootstrap-table';
import 'react-bootstrap-table/css/react-bootstrap-table.css';
import YBPanelItem from './YBPanelItem';

export default class NodeDetails extends Component {
  static propTypes = {
    nodeDetails: PropTypes.object.isRequired
  };

  render() {
    const { nodeDetails } = this.props;
	  const nodeDetailRows = Object.keys(nodeDetails).map(function(key) {
      var nodeDetail = nodeDetails[key];
      return {
        name: nodeDetail.nodeName,
        regionAz: `${nodeDetail.cloudInfo.region}/${nodeDetail.cloudInfo.az}`,
        isMaster: nodeDetail.isMaster ? "Yes" : "No",
        isTServer: nodeDetail.isTserver ? "Yes" : "No",
        privateIP: nodeDetail.cloudInfo.private_ip,
        nodeStatus: nodeDetail.state,
      };
    });

    return (
      <YBPanelItem name="Node Details">
        <BootstrapTable data={nodeDetailRows}>
          <TableHeaderColumn dataField="name" isKey={true}>Instance Name</TableHeaderColumn>
          <TableHeaderColumn dataField="regionAz">Region/Zone</TableHeaderColumn>
          <TableHeaderColumn dataField="isMaster">Master</TableHeaderColumn>
          <TableHeaderColumn dataField="isTServer">TServer</TableHeaderColumn>
          <TableHeaderColumn dataField="privateIP">Private IP</TableHeaderColumn>
          <TableHeaderColumn dataField="nodeStatus">Node Status</TableHeaderColumn>
        </BootstrapTable>
      </YBPanelItem>
    )
  }
}
