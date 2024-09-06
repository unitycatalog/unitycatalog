import { useState } from 'react';
import { Button } from 'antd';
import CreateSchemaModal from '../modals/CreateSchemaModal';

interface CreateSchemaActionProps {
  catalog: string;
}

export default function CreateSchemaAction({
  catalog,
}: CreateSchemaActionProps) {
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button type="primary" onClick={() => setOpen(true)}>
        Create Schema
      </Button>
      <CreateSchemaModal
        open={open}
        closeModal={() => setOpen(false)}
        catalog={catalog}
      />
    </>
  );
}
