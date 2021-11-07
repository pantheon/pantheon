import { styled } from "@operational/components";

/**
 * This component defines the base styles applied to layout components for query interfaces
 * (native, aggregate, record etc.). In order to avoid overflow/stretched layout problems,
 * these layout components have an internal div that is positioned absolute to cover the grid cell.
 */
const BaseLayout = styled("div")(({ theme }) => ({
  display: "grid",
  gridColumnGap: theme.space.small,
  gridRowGap: theme.space.small,
  padding: theme.space.small,
  // Slots
  "& > div": {
    position: "relative",
  },
  // Cards
  "& > div > div": {
    margin: 0,
    position: "absolute",
    overflow: "auto",
    top: 0,
    left: 0,
    bottom: 0,
    right: 0,
  },
}));

export default BaseLayout;
