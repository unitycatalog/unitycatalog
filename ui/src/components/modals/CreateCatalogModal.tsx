import { Button, Form, Input, Modal, Typography } from 'antd';
import {
  CreateCatalogMutationParams,
  useCreateCatalog,
} from '../../hooks/catalog';
import { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { useNavigate } from 'react-router-dom';

interface CreateCatalogModalProps {
  open: boolean;
  closeModal: () => void;
}

export function CreateCatalogModal({
  open,
  closeModal,
}: CreateCatalogModalProps) {
  const navigate = useNavigate();
  const mutation = useCreateCatalog({
    onSuccessCallback: (catalog) => {
      navigate(`/data/${catalog.name}`);
    },
  });
  const submitRef = useRef<HTMLButtonElement>(null);

  const handleSubmit = useCallback(() => {
    submitRef.current?.click();
  }, []);

  return (
    <Modal
      title={<Typography.Title level={4}>Create catalog</Typography.Title>}
      okText="Create"
      cancelText="Cancel"
      open={open}
      destroyOnClose
      onCancel={closeModal}
      onOk={handleSubmit}
      okButtonProps={{ loading: mutation.isPending }}
    >
      <Typography.Paragraph type="secondary">
        Create catalog description
      </Typography.Paragraph>
      <Form<CreateCatalogMutationParams>
        layout="vertical"
        onFinish={(values) => {
          mutation.mutate(values);
        }}
        name="Create catalog form"
      >
        <Form.Item
          label={<Typography.Text strong>Name</Typography.Text>}
          name="name"
        >
          <Input />
        </Form.Item>
        <Form.Item
          label={<Typography.Text strong>Comment</Typography.Text>}
          name="comment"
        >
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
