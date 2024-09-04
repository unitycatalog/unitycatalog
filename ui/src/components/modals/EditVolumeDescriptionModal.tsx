import { Button, Form, Modal, Typography } from 'antd';
import React, { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { UpdateVolumeMutationParams } from '../../hooks/volumes';

interface EditVolumeDescriptionModalProps {
  open: boolean;
  volume: UpdateVolumeMutationParams;
  closeModal: () => void;
  onSubmit: (comment: UpdateVolumeMutationParams) => void;
  loading: boolean;
}

export function EditVolumeDescriptionModal({
  open,
  volume,
  closeModal,
  onSubmit,
  loading,
}: EditVolumeDescriptionModalProps) {
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
        Volume description
      </Typography.Paragraph>
      <Form<UpdateVolumeMutationParams>
        layout="vertical"
        onFinish={(values) => {
          onSubmit(values);
        }}
        name="Edit description form"
        initialValues={{ comment: volume.comment }}
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
