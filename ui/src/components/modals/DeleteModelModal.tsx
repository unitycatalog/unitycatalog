import { Modal, Typography } from 'antd';
import React, { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useNotification } from '../../utils/NotificationContext';
import { useDeleteModel } from '../../hooks/models';
import { SchemaTabs } from '../../pages/SchemaDetails';

interface DeleteModelModalProps {
  open: boolean;
  closeModal: () => void;
  catalog: string;
  schema: string;
  model: string;
  existingVersions: boolean;
}

export function DeleteModelModal({
  open,
  closeModal,
  catalog,
  schema,
  model,
  existingVersions,
}: DeleteModelModalProps) {
  const navigate = useNavigate();
  const { setNotification } = useNotification();
  const mutation = useDeleteModel({
    catalog,
    schema,
    model,
  });

  const handleSubmit = useCallback(() => {
    mutation.mutate(
      {
        catalog_name: catalog,
        schema_name: schema,
        name: model,
      },
      {
        onError: (error: Error) => {
          setNotification(error.message, 'error');
        },
        onSuccess: () => {
          setNotification(`${model} model successfully deleted`, 'success');
          navigate(`/data/${catalog}/${schema}`, {
            state: { tab: SchemaTabs.Models },
          });
        },
      },
    );
  }, [mutation, catalog, schema, model, setNotification, navigate]);

  return (
    <Modal
      title={
        <Typography.Title type={'danger'} level={4}>
          Delete model
        </Typography.Title>
      }
      okText="Delete"
      okType="danger"
      cancelText="Cancel"
      open={open}
      destroyOnClose
      onCancel={closeModal}
      onOk={handleSubmit}
      okButtonProps={{
        loading: mutation.isPending,
        disabled: existingVersions,
      }}
    >
      {existingVersions ? (
        <Typography.Text>
          Registered models cannot be deleted with existing model versions.
          Delete model versions and try again.
        </Typography.Text>
      ) : (
        <div>
          <Typography.Text>
            Are you sure you want to delete model
          </Typography.Text>
          <Typography.Text strong>{` ${model}`}</Typography.Text>
          <Typography.Text>? This operation cannot be undone.</Typography.Text>
        </div>
      )}
    </Modal>
  );
}
