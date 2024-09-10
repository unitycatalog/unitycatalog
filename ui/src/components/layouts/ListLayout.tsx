import { SearchOutlined } from '@ant-design/icons';
import { Col, Flex, Input, Row, Table, TableColumnsType } from 'antd';
import { AnyObject } from 'antd/es/_util/type';
import { ReactNode, useMemo, useState } from 'react';
import styles from './ListLayout.module.css';

interface ListLayoutProps<T> {
  data: T[] | undefined;
  columns: TableColumnsType<T>;
  title: ReactNode;
  onRowClick?: (record: T) => void;
  loading?: boolean;
  filters?: ReactNode;
  showSearch?: boolean;
}

export default function ListLayout<T extends AnyObject = AnyObject>({
  data,
  columns,
  title,
  onRowClick,
  loading,
  filters,
  showSearch = true,
}: ListLayoutProps<T>) {
  const [filterValue, setFilterValue] = useState('');

  const filteredData = useMemo(() => {
    if (!filterValue) return data;
    return data?.filter((item) =>
      Object.values<string | boolean | number | null | undefined>(item).some(
        (value) =>
          String(value).toLowerCase().includes(filterValue.toLowerCase()),
      ),
    );
  }, [data, filterValue]);

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
        loading={loading}
        className={onRowClick ? styles.clickableListLayout : undefined}
        dataSource={filteredData}
        columns={columns}
        pagination={{
          hideOnSinglePage: true,
          pageSize: 10,
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
