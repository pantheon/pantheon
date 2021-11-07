import { CardSection, CardSectionProps } from "@operational/components";
import React, { useState } from "react";

export const PslSidenavSection: React.SFC<CardSectionProps> = ({ title, ...props }) => {
  const [isCollapsed, setIsCollapsed] = useState(false);
  return <CardSection collapsed={isCollapsed} onToggle={() => setIsCollapsed(!isCollapsed)} title={title} {...props} />;
};
