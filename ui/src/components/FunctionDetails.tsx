import React from 'react';
import { useParams } from 'react-router-dom';

export default function FunctionDetails() {
  const { catalog, schema, ucFunction } = useParams();
  return (
    <div>
      FunctionDetails: {catalog}.{schema}.{ucFunction}
    </div>
  );
}
