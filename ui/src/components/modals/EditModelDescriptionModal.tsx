import { Button, Form, Modal, Typography } from 'antd';
import React, { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { UpdateModelMutationParams } from '../../hooks/models';

interface EditModelDescriptionModalProps {
  open: boolean;
  model: UpdateModelMutationParams;
  closeModal: () => void;
  onSubmit: (model: UpdateModelMutationParams) => void;
  loading: boolean;
}

export function EditModelDescriptionModal({
  open,
  model,
  closeModal,
  onSubmit,
  loading,
}: EditModelDescriptionModalProps) {
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
        Model description
      </Typography.Paragraph>
      <Form<UpdateModelMutationParams>
        layout="vertical"
        onFinish={(values) => {
          onSubmit(values);
        }}
        name="Edit description form"
        initialValues={{ comment: model.comment }}
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
