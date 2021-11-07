import { Button } from "@operational/components";
import React from "react";

export interface EditButtonProps {
  onClick: () => void;
}

export const EditButton: React.SFC<EditButtonProps> = ({ onClick, ...props }) => (
  <Button {...props} condensed color="primary" onClick={onClick}>
    Edit
  </Button>
);

export default EditButton;
