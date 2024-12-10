import { Breadcrumb, Col, Flex, Grid, Row } from 'antd';
import { BreadcrumbItemType } from 'antd/es/breadcrumb/Breadcrumb';
import React, { ReactNode } from 'react';
import { getChildOnType } from '../../utils/getChildOnType';

interface DetailsLayoutProps {
  title: ReactNode;
  breadcrumbs?: BreadcrumbItemType[];
  children: ReactNode;
}

const { useBreakpoint } = Grid;

function DetailsLayout({ title, breadcrumbs, children }: DetailsLayoutProps) {
  const screens = useBreakpoint();

  const contentChild = getChildOnType(children, Content);
  const asideChild = getChildOnType(children, Aside);

  return (
    <Flex vertical gap="middle" style={{ flexGrow: 1 }}>
      <Row>
        <Col span={24}>{breadcrumbs && <Breadcrumb items={breadcrumbs} />}</Col>
        <Col span={24}>{title}</Col>
      </Row>
      <Row
        style={{
          borderTop: '1px solid lightgrey',
          flexGrow: 1,
          flexDirection: screens.lg ? 'row' : 'column',
        }}
      >
        <Col
          xs={{ order: 2 }}
          lg={{ order: 1, span: 16 }}
          style={{
            paddingTop: 16,
            paddingRight: screens.lg ? 16 : 0,
            flexGrow: 1,
          }}
        >
          {contentChild}
        </Col>
        <Col
          xs={{ order: 1 }}
          lg={{ order: 2, span: 8 }}
          style={{
            paddingTop: 16,
            paddingLeft: screens.lg ? 16 : 0,
            borderLeft: screens.lg ? '1px solid lightgrey' : 'none',
          }}
        >
          {asideChild}
        </Col>
      </Row>
    </Flex>
  );
}

function Content({ children }: { children: ReactNode }) {
  return <>{children}</>;
}

DetailsLayout.Content = Content;

function Aside({ children }: { children: ReactNode }) {
  return <>{children}</>;
}

DetailsLayout.Aside = Aside;

export default DetailsLayout;
