import { Typography } from 'antd';

interface DescriptionBoxProps {
  comment: string;
}

export default function DescriptionBox({ comment }: DescriptionBoxProps) {
  return (
    <div>
      <Typography.Title level={5}>Description</Typography.Title>
      <Typography.Text type="secondary">{comment}</Typography.Text>
    </div>
  );
}
