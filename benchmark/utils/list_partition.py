from typing import List, TypeVar

T = TypeVar('T')


class ListPartition:

    @staticmethod
    def partition_list(origin_list: List[T], size: int) -> List[List[T]]:
        """
        partition a list to specified size.

        :param origin_list: the original list to partition
        :param size: the partition size
        :return: the partitioned list
        """
        result_list = []

        if origin_list is None or len(origin_list) == 0 or size <= 0:
            return result_list

        if len(origin_list) <= size:
            for item in origin_list:
                result_item_list = [item]
                result_list.append(result_item_list)

            for i in range(size - len(origin_list)):
                result_list.append([])

            return result_list

        for i in range(size):
            result_list.append([])

        count = 0
        for item in origin_list:
            index = count % size
            result_list[index].append(item)
            count += 1

        return result_list
