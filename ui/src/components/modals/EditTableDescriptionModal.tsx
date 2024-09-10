import { Button, Form, Modal, Typography } from 'antd';
import React, { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { UpdateTableMutationParams } from '../../hooks/tables';

interface EditTableDescriptionModalProps {
  open: boolean;
  table: UpdateTableMutationParams;
  closeModal: () => void;
  onSubmit: (comment: UpdateTableMutationParams) => void;
  loading: boolean;
}

export function EditTableDescriptionModal({
  open,
  table,
  closeModal,
  onSubmit,
  loading,
}: EditTableDescriptionModalProps) {
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
        Table description
      </Typography.Paragraph>
      <Form<UpdateTableMutationParams>
        layout="vertical"
        onFinish={(values) => {
          onSubmit(values);
        }}
        name="Edit description form"
        initialValues={{ comment: table.comment }}
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
