import React from 'react';
import { useParams } from 'react-router-dom';

export default function VolumeDetails() {
  const { catalog, schema, volume } = useParams();
  return (
    <div>
      VolumeDetails: {catalog}.{schema}.{volume}
    </div>
  );
}
