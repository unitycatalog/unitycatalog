import React from 'react';

export function getChildOnDisplayName(
  children: React.ReactNode[] | React.ReactNode,
  displayName: string,
) {
  return React.Children.toArray(children).find((child) => {
    if (React.isValidElement(child)) {
      return typeof child.type === 'function'
        ? child.type.name === displayName
        : false;
    }
    return false;
  });
}
