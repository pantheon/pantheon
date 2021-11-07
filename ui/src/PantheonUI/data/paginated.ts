import * as tucson from "tucson-decode";

export const regularPageSize = 20;

export interface Paginated<T> {
  page: {
    current: number;
    itemCount: number;
    itemsPerPage: number;
  };
  data: T;
}

const pageDecoder = tucson.object({
  current: tucson.number,
  itemCount: tucson.number,
  itemsPerPage: tucson.number,
});

export const pageQueryParams = (page?: number) =>
  typeof page === "number"
    ? {
        page,
        pageSize: regularPageSize,
      }
    : {};

export const paginatedDecoder = <T>(resourceDecoder: tucson.Decoder<T>): tucson.Decoder<Paginated<T>> =>
  tucson.object({
    page: pageDecoder,
    data: resourceDecoder,
  });
