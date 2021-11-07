// tslint:disable-next-line:no-implicit-dependencies
import { keyframes } from "@emotion/core";
import { styled } from "@operational/components";

const fadeIn = keyframes`
  from{opacity: 0}
  to{opacity: 1}
`;

export const DropDisabledEffect = styled.div`
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  background-color: rgba(255, 255, 255, 0.3);
  animation: ${fadeIn} 0.2s linear;
`;
