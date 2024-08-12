import { Button, Form, Modal, Typography } from 'antd';
import React, { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { UpdateCatalogMutationParams } from '../../hooks/catalog';

interface EditCatalogDescriptionModalProps {
  open: boolean;
  catalog: UpdateCatalogMutationParams;
  closeModal: () => void;
  onSubmit: (comment: UpdateCatalogMutationParams) => void;
  loading: boolean;
}

export function EditCatalogDescriptionModal({
  open,
  catalog,
  closeModal,
  onSubmit,
  loading,
}: EditCatalogDescriptionModalProps) {
  const submitRef = useRef<HTMLButtonElement>(null);

  const handleSubmit = useCallback(() => {
    submitRef.current?.click();
  }, []);

  return (
    <Modal
      title={<Typography.Title level={4}>Edit description</Typography.Title>}
      okText="Save"
      cancelText="Cancel"
      open={open}
      destroyOnClose
      onCancel={closeModal}
      onOk={handleSubmit}
      okButtonProps={{ loading: loading }}
    >
      <Typography.Paragraph type="secondary">
        Catalog description
      </Typography.Paragraph>
      <Form<UpdateCatalogMutationParams>
        layout="vertical"
        onFinish={(values) => {
          onSubmit(values);
        }}
        name="Edit description form"
        initialValues={{ comment: catalog.comment }}
      >
        <Form.Item name="comment">
          <TextArea />
        </Form.Item>
        <Form.Item hidden>
          <Button type="primary" htmlType="submit" ref={submitRef}>
            Create
          </Button>
        </Form.Item>
      </Form>
    </Modal>
  );
}
