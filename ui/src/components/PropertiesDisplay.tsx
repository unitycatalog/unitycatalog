import { Flex, Tag, Typography } from 'antd';

interface PropertiesDisplayProps {
  properties?: { [key: string]: string };
}

export default function PropertiesDisplay({
  properties,
}: PropertiesDisplayProps) {
  if (!properties || Object.keys(properties).length === 0) return null;

  return (
    <Flex vertical gap="middle">
      <Typography.Title level={5}>Properties</Typography.Title>
      {Object.entries(properties).map(([key, value]) => (
        <div key={key}>
          <Typography.Text strong>{key}: </Typography.Text>
          <Tag>{value}</Tag>
        </div>
      ))}
    </Flex>
  );
}
