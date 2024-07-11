import { Flex, Typography } from 'antd';
import { ReactNode } from 'react';

export type MetadataListType<T> = Array<{
  key: string;
  dataIndex: keyof T;
  label?: string;
  render?: (value: any) => ReactNode;
}>;

interface MetadataListProps<T> {
  data: T;
  metadata: MetadataListType<T>;
  title: string;
}

export default function MetadataList<
  T extends Record<K, string | number | null>,
  K extends keyof T
>({ data, metadata, title }: MetadataListProps<T>) {
  return (
    <Flex vertical gap="middle">
      <Typography.Title level={5}>{title}</Typography.Title>
      {metadata.map(({ key, label, dataIndex, render }) => {
        const value = data[dataIndex];
        if (!value) return null;

        return (
          <div key={key}>
            <Typography.Text strong>{label ?? key}: </Typography.Text>
            {render?.(value) ?? value}
          </div>
        );
      })}
    </Flex>
  );
}
