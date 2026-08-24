import { Modal, Typography } from 'antd';
import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useNotification } from '../../utils/NotificationContext';
import { useDeleteModelVersion } from '../../hooks/models';

interface DeleteModelVersionModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
  schema: string;
  model: string;
  version: number;
}

export function DeleteModelVersionModal({
  open,
  closeModal,
  catalog,
  schema,
  model,
  version,
}: DeleteModelVersionModalProps) {
  const navigate = useNavigate();
  const { setNotification } = useNotification();
  const mutation = useDeleteModelVersion({
    full_name: [catalog, schema, model].join('.'),
    version: Number(version),
  });

  const handleSubmit = useCallback(() => {
    mutation.mutate(
      {
        full_name: [catalog, schema, model].join('.'),
        version: Number(version),
      },
      {
        onError: (error: Error) => {
          setNotification(error.message, 'error');
        },
        onSuccess: () => {
          setNotification(
            `Model version ${version} successfully deleted`,
            'success',
          );
          navigate(`/models/${catalog}/${schema}/${model}`);
        },
      },
    );
  }, [mutation, catalog, schema, model, version, setNotification, navigate]);

  return (
    <Modal
      title={
        <Typography.Title type={'danger'} level={4}>
          Delete model version
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
        Are you sure you want to delete model version
      </Typography.Text>
      <Typography.Text strong>{` ${version}`}</Typography.Text>
      <Typography.Text>? This operation cannot be undone.</Typography.Text>
    </Modal>
  );
}
