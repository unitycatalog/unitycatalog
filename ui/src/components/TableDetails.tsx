import React from 'react';
import { useParams } from 'react-router-dom';

export default function TableDetails() {
  const { catalog, schema, table } = useParams();

  return (
    <div>
      TableDetails: {catalog}.{schema}.{table}
    </div>
  );
}
