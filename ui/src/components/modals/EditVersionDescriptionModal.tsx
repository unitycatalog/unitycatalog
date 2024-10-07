import { Button, Form, Modal, Typography } from 'antd';
import React, { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { UpdateModelVersionMutationParams } from '../../hooks/models';

interface EditVersionDescriptionModalProps {
  open: boolean;
  version: UpdateModelVersionMutationParams;
  closeModal: () => void;
  onSubmit: (version: UpdateModelVersionMutationParams) => void;
  loading: boolean;
}

export function EditVersionDescriptionModal({
  open,
  version,
  closeModal,
  onSubmit,
  loading,
}: EditVersionDescriptionModalProps) {
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
        Model version description
      </Typography.Paragraph>
      <Form<UpdateModelVersionMutationParams>
        layout="vertical"
        onFinish={(values) => {
          onSubmit(values);
        }}
        name="Edit description form"
        initialValues={{ comment: version.comment }}
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
