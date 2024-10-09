import { Button, Form, Input, Modal, Typography } from 'antd';
import { useCallback, useRef } from 'react';
import TextArea from 'antd/es/input/TextArea';
import { useNavigate } from 'react-router-dom';
import { useNotification } from '../../utils/NotificationContext';
import { CreateModelMutationParams, useCreateModel } from '../../hooks/models';
import { SchemaTabs } from '../../pages/SchemaDetails';

interface CreateModelModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
  schema: string;
}

export default function CreateModelModal({
  open,
  closeModal,
  catalog,
  schema,
}: CreateModelModalProps) {
  const navigate = useNavigate();
  const mutation = useCreateModel();
  const { setNotification } = useNotification();
  const submitRef = useRef<HTMLButtonElement>(null);

  const handleSubmit = useCallback(() => {
    submitRef.current?.click();
  }, []);

  return (
    <Modal
      title={
        <Typography.Title level={4}>Create registered model</Typography.Title>
      }
      okText="Create"
      cancelText="Cancel"
      open={open}
      destroyOnClose
      onCancel={closeModal}
      onOk={handleSubmit}
      okButtonProps={{ loading: mutation.isPending }}
    >
      <Form<CreateModelMutationParams>
        layout="vertical"
        onFinish={(values) => {
          mutation.mutate(values, {
            onError: (error) => {
              setNotification(error.message, 'error');
            },
            onSuccess: (model) => {
              closeModal();
              navigate(`/data/${model.catalog_name}/${model.schema_name}`, {
                state: { tab: SchemaTabs.Models },
              });
              setNotification(
                `Model ${model.name} successfully created`,
                'success',
              );
            },
          });
        }}
        name="Create schema form"
        initialValues={{ catalog_name: catalog, schema_name: schema }}
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
        <Form.Item name="schema_name" hidden>
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
