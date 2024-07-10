import React from 'react';
import { useParams } from 'react-router-dom';

export default function SchemaDetails() {
  const { catalog, schema } = useParams();

  return (
    <div>
      SchemaDetails: {catalog}.{schema}
    </div>
  );
}
