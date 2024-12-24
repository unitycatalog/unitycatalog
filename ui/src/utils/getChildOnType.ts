import React from 'react';

export function getChildOnType(
  children: React.ReactNode[] | React.ReactNode,
  type: React.JSXElementConstructor<any>,
) {
  return React.Children.toArray(children).find((child) => {
    if (React.isValidElement(child)) {
      return child.type === type;
    }
    return false;
  });
}
