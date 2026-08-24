import { ModelVersionStatus } from '../../hooks/models';
import { Tooltip, Typography } from 'antd';
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  MinusCircleOutlined,
} from '@ant-design/icons';
import React from 'react';

export default function ModelVersionStatusDisplay({
  status,
}: {
  status: ModelVersionStatus;
}) {
  switch (status) {
    case ModelVersionStatus.READY:
      return (
        <Tooltip title={`READY`}>
          <CheckCircleOutlined style={{ fontSize: '18px', color: 'green' }} />
        </Tooltip>
      );
    case ModelVersionStatus.PENDING_REGISTRATION:
      return (
        <Tooltip title={`PENDING REGISTRATION`}>
          <MinusCircleOutlined style={{ fontSize: '18px', color: 'gray' }} />
        </Tooltip>
      );
    case ModelVersionStatus.FAILED_REGISTRATION:
      return (
        <Tooltip title={`FAILED REGISTRATION`}>
          <CloseCircleOutlined style={{ fontSize: '18px', color: 'red' }} />
        </Tooltip>
      );
    // NOTE:
    // As of [25/11/2024], `MODEL_VERSION_STATUS_UNKNOWN` is not handled here.
    // If exhaustive handling of the status is required, add `assertNever(status)` here.
    default:
      return <Typography.Text type="secondary">UNKNOWN</Typography.Text>;
  }
}
