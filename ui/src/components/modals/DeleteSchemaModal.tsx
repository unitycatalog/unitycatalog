import { Modal, Typography } from 'antd';
import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useNotification } from '../../utils/NotificationContext';
import { useDeleteSchema } from '../../hooks/schemas';

interface DeleteSchemaModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
  schema: string;
}

export function DeleteSchemaModal({
  open,
  closeModal,
  catalog,
  schema,
}: DeleteSchemaModalProps) {
  const navigate = useNavigate();
  const { setNotification } = useNotification();
  const mutation = useDeleteSchema({
    full_name: [catalog, schema].join('.'),
  });

  const handleSubmit = useCallback(() => {
    mutation.mutate(
      { full_name: [catalog, schema].join('.') },
      {
        onError: (error: Error) => {
          setNotification(error.message, 'error');
        },
        onSuccess: () => {
          setNotification(`${schema} schema successfully deleted`, 'success');
          navigate(`/data/${catalog}`);
        },
      },
    );
  }, [mutation, catalog, schema, setNotification, navigate]);

  return (
    <Modal
      title={
        <Typography.Title type={'danger'} level={4}>
          Delete schema
        </Typography.Title>
      }
      okText="Delete"
      okType="danger"
      cancelText="Cancel"
      open={open}
      destroyOnClose
      onCancel={closeModal}
      onOk={handleSubmit}
      okButtonProps={{ loading: mutation.isPending }}
    >
      <Typography.Text>
        Are you sure you want to delete the schema
      </Typography.Text>
      <Typography.Text strong>{` ${schema}`}</Typography.Text>
      <Typography.Text>? This operation cannot be undone.</Typography.Text>
    </Modal>
  );
}
