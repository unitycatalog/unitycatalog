import { Modal, Typography } from 'antd';
import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useDeleteFunction } from '../../hooks/functions';
import { useNotification } from '../../utils/NotificationContext';

interface DeleteFunctionModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
  schema: string;
  ucFunction: string;
}

export function DeleteFunctionModal({
  open,
  closeModal,
  catalog,
  schema,
  ucFunction,
}: DeleteFunctionModalProps) {
  const navigate = useNavigate();
  const { setNotification } = useNotification();
  const mutation = useDeleteFunction({
    name: [catalog, schema, ucFunction].join('.'),
  });

  const handleSubmit = useCallback(() => {
    mutation.mutate(
      { name: [catalog, schema, ucFunction].join('.') },
      {
        onError: (error: Error) => {
          setNotification(error.message, 'error');
        },
        onSuccess: () => {
          setNotification(
            `${ucFunction} function successfully deleted`,
            'success',
          );
          navigate(`/data/${catalog}/${schema}`);
        },
      },
    );
  }, [mutation, ucFunction, setNotification, navigate, catalog, schema]);

  return (
    <Modal
      title={
        <Typography.Title type={'danger'} level={4}>
          Delete function
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
        Are you sure you want to delete the function
      </Typography.Text>
      <Typography.Text strong>{` ${ucFunction}`}</Typography.Text>
      <Typography.Text>? This operation cannot be undone.</Typography.Text>
    </Modal>
  );
}
