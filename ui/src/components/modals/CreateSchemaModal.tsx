import { Button, Form, Input, Modal, Typography } from 'antd';
import { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { useNavigate } from 'react-router-dom';
import {
  CreateSchemaMutationParams,
  useCreateSchema,
} from '../../hooks/schemas';
import { useNotification } from '../../utils/NotificationContext';

interface CreateSchemaModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
}

export default function CreateSchemaModal({
  open,
  closeModal,
  catalog,
}: CreateSchemaModalProps) {
  const navigate = useNavigate();
  const mutation = useCreateSchema();
  const { setNotification } = useNotification();
  const submitRef = useRef<HTMLButtonElement>(null);

  const handleSubmit = useCallback(() => {
    submitRef.current?.click();
  }, []);

  return (
    <Modal
      title={<Typography.Title level={4}>Create schema</Typography.Title>}
      okText="Create"
      cancelText="Cancel"
      open={open}
      destroyOnClose
      onCancel={closeModal}
      onOk={handleSubmit}
      okButtonProps={{ loading: mutation.isPending }}
    >
      <Typography.Paragraph type="secondary">
        Create schema description
      </Typography.Paragraph>
      <Form<CreateSchemaMutationParams>
        layout="vertical"
        onFinish={(values) => {
          mutation.mutate(values, {
            onError: (error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (schema) => {
              navigate(`/data/${schema.catalog_name}/${schema.name}`);
            },
          });
        }}
        name="Create schema form"
        initialValues={{ catalog_name: catalog }}
      >
        <Form.Item
          required
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
        <Form.Item name="catalog_name" hidden>
          <Input />
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
