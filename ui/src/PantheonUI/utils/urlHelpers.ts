export const removeQuery = (url: string): string => {
  const questionMarkIndex = url.indexOf("?");
  if (questionMarkIndex === -1) {
    return url;
  }
  return url.slice(0, questionMarkIndex);
};
