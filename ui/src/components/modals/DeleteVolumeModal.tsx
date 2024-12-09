import { Modal, Typography } from 'antd';
import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDeleteVolume } from '../../hooks/volumes';
import { useNotification } from '../../utils/NotificationContext';

interface DeleteVolumeModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
  schema: string;
  volume: string;
}

export function DeleteVolumeModal({
  open,
  closeModal,
  catalog,
  schema,
  volume,
}: DeleteVolumeModalProps) {
  const navigate = useNavigate();
  const { setNotification } = useNotification();
  const mutation = useDeleteVolume({
    name: [catalog, schema, volume].join('.'),
  });

  const handleSubmit = useCallback(() => {
    mutation.mutate(
      { name: [catalog, schema, volume].join('.') },
      {
        onError: (error: Error) => {
          setNotification(error.message, 'error');
        },
        onSuccess: () => {
          setNotification(`${volume} volume successfully deleted`, 'success');
          navigate(`/data/${catalog}/${schema}`);
        },
      },
    );
  }, [mutation, volume, setNotification, navigate, catalog, schema]);

  return (
    <Modal
      title={
        <Typography.Title type={'danger'} level={4}>
          Delete volume
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
        Are you sure you want to delete the volume
      </Typography.Text>
      <Typography.Text strong>{` ${volume}`}</Typography.Text>
      <Typography.Text>? This operation cannot be undone.</Typography.Text>
    </Modal>
  );
}
