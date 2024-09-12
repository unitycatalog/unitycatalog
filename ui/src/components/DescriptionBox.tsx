import { Button, Typography } from 'antd';
import { EditOutlined } from '@ant-design/icons';

interface DescriptionBoxProps {
  comment: string;
  onEdit?: () => void;
}

export default function DescriptionBox({
  comment,
  onEdit,
}: DescriptionBoxProps) {
  return (
    <div>
      <Typography.Title level={5}>
        Description {comment && <EditOutlined onClick={onEdit} />}
      </Typography.Title>
      <Typography.Text type="secondary">{comment}</Typography.Text>
      {!comment && <Button onClick={onEdit}>Add description</Button>}
    </div>
  );
}
