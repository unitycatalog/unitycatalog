import { useState } from 'react';
import { CreateCatalogModal } from '../modals/CreateCatalogModal';
import { Button } from 'antd';

export default function CreateCatalogAction() {
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button type="primary" onClick={() => setOpen(true)}>
        Create Catalog
      </Button>
      <CreateCatalogModal open={open} closeModal={() => setOpen(false)} />
    </>
  );
}
