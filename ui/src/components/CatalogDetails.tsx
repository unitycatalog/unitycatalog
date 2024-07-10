import React from 'react';
import { useParams } from 'react-router-dom';

export default function CatalogDetails() {
  const { catalog } = useParams();

  return <div>CatalogDetails: {catalog}</div>;
}
