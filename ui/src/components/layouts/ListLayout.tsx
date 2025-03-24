import { SearchOutlined } from '@ant-design/icons';
import {
  Col,
  Flex,
  Input,
  Row,
  Table,
  TableProps,
  TableColumnsType,
} from 'antd';
import { AnyObject } from 'antd/es/_util/type';
import { ReactNode, useMemo, useState } from 'react';
import styles from './ListLayout.module.css';

interface ListLayoutProps<T> {
  data: T[] | undefined;
  columns: TableColumnsType<T>;
  title: ReactNode;
  rowKey: TableProps['rowKey'];
  onRowClick?: (record: T) => void;
  loading?: boolean;
  filters?: ReactNode;
  showSearch?: boolean;
}

export default function ListLayout<T extends AnyObject = AnyObject>({
  data,
  columns,
  title,
  rowKey,
  onRowClick,
  loading,
  filters,
  showSearch = true,
}: ListLayoutProps<T>) {
  const [filterValue, setFilterValue] = useState('');
  const [pageSize, setPageSize] = useState(10);

  const filteredData = useMemo(() => {
    if (!filterValue) return data;
    return data?.filter((item) =>
      String(item.name).toLowerCase().includes(filterValue.toLowerCase()),
    );
  }, [data, filterValue]);

  const onShowSizeChange = (current: number, pageSize: number) => {
    setPageSize(pageSize);
  };

  return (
    <Flex gap="middle" vertical style={{ flexGrow: 1 }}>
      {title}
      {showSearch && (
        <Row gutter={[8, 8]}>
          <Col
            span={8}
            xs={{ span: 12 }}
            md={{ span: 10 }}
            lg={{ span: 8 }}
            xl={{ span: 6 }}
          >
            <Input
              placeholder="Search"
              prefix={<SearchOutlined />}
              value={filterValue}
              onChange={(e) => setFilterValue(e.target.value)}
            />
          </Col>
          {filters && <Col flex={1}>{filters}</Col>}
        </Row>
      )}
      <Table
        rowKey={rowKey}
        loading={loading}
        className={onRowClick ? styles.clickableListLayout : undefined}
        dataSource={filteredData}
        columns={columns}
        pagination={{
          hideOnSinglePage: true,
          pageSize: pageSize,
          onShowSizeChange: onShowSizeChange,
          showTotal: (total, range) => `${range[0]}-${range[1]} of ${total}`,
        }}
        onRow={(row) => {
          return {
            onClick: () => onRowClick?.(row),
          };
        }}
      />
    </Flex>
  );
}
