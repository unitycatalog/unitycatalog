import React from 'react';
import { useListCatalogs } from '../hooks/catalog';

export default function CatalogsList() {
  const listCatalogsRequest = useListCatalogs();

  return (
    <div>
      {listCatalogsRequest.data?.catalogs.map((catalog: any) => {
        return <li>{catalog.name}</li>;
      })}
    </div>
  );
}
