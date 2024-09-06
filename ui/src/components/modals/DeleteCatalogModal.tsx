import { Modal, Typography } from 'antd';
import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDeleteCatalog } from '../../hooks/catalog';
import { useNotification } from '../../utils/NotificationContext';

interface DeleteCatalogModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
}

export function DeleteCatalogModal({
  open,
  closeModal,
  catalog,
}: DeleteCatalogModalProps) {
  const navigate = useNavigate();
  const { setNotification } = useNotification();
  const mutation = useDeleteCatalog();

  const handleSubmit = useCallback(() => {
    mutation.mutate(
      { name: catalog },
      {
        onError: (error: Error) => {
          setNotification(error.message, 'error');
        },
        onSuccess: () => {
          setNotification(`${catalog} catalog successfully deleted`, 'success');
          navigate(`/`);
        },
      },
    );
  }, [mutation, catalog, setNotification, navigate]);

  return (
    <Modal
      title={
        <Typography.Title type={'danger'} level={4}>
          Delete catalog
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
        Are you sure you want to delete the catalog
      </Typography.Text>
      <Typography.Text strong>{` ${catalog}`}</Typography.Text>
      <Typography.Text>? This operation cannot be undone.</Typography.Text>
    </Modal>
  );
}
