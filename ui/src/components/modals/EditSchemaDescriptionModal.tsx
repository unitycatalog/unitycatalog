import { Button, Form, Modal, Typography } from 'antd';
import React, { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { UpdateSchemaMutationParams } from '../../hooks/schemas';

interface EditSchemaDescriptionModalProps {
  open: boolean;
  schema: UpdateSchemaMutationParams;
  closeModal: () => void;
  onSubmit: (comment: UpdateSchemaMutationParams) => void;
  loading: boolean;
}

export function EditSchemaDescriptionModal({
  open,
  schema,
  closeModal,
  onSubmit,
  loading,
}: EditSchemaDescriptionModalProps) {
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
        Schema description
      </Typography.Paragraph>
      <Form<UpdateSchemaMutationParams>
        layout="vertical"
        onFinish={(values) => {
          onSubmit(values);
        }}
        name="Edit description form"
        initialValues={{ comment: schema.comment }}
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
