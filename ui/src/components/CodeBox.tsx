import { Typography } from 'antd';
import React from 'react';

interface CodeBoxProps {
  definition: string;
}

export default function CodeBox({ definition }: CodeBoxProps) {
  return (
    <div>
      <Typography.Title level={5}>Definition</Typography.Title>
      <code
        style={{
          margin: 4,
          padding: 4,
          display: 'block',
          background: 'rgba(150, 150, 150, 0.1)',
          border: '1px solid lightgray',
          borderRadius: '3px',
        }}
      >
        {definition.split('\\n').map((subString, index) => (
          <React.Fragment key={index}>
            {subString}
            <br />
          </React.Fragment>
        ))}
      </code>
    </div>
  );
}
