import os
from collections import namedtuple
from pprint import pprint

PATH = f'./sample'

Tags = namedtuple('Tags', ['input', 'etl', 'reactor', 'owner'])


def receive_all_sql_files(path: str) -> list:
    list_of_files = []
    files_in_dir = os.listdir(path)
    for file in files_in_dir:
        if file.endswith('.sql'):
            list_of_files.append(path + '/' + file)
        else:
            try:
                list_of_files = concat_lists(list_of_files, receive_all_sql_files(path + '/' + file))
            except Exception:
                continue
    return list_of_files


def concat_lists(list1: list, list2: list) -> list:
    for each_elem in list2:
        list1.append(each_elem)
    return list(set(list1))


def concat_tags(tags_tuple_1: Tags, tags_tuple_2: Tags) -> Tags:
    return Tags(input=concat_lists(tags_tuple_1.input, tags_tuple_2.input),
                etl=concat_lists(tags_tuple_1.etl, tags_tuple_2.etl),
                reactor=concat_lists(tags_tuple_1.reactor, tags_tuple_2.reactor),
                owner=concat_lists(tags_tuple_1.owner, tags_tuple_2.owner)
                )


def parse_tags(list_of_files: list) -> dict:
    d = {}
    for file in list_of_files:
        with open(file, 'r') as f:
            input_list = []
            output_list = []
            etl_list = []
            reactor_list = []
            owner_list = []

            text = f.read()
            tags_part_text = text[:text.find('*/')]
            for line in tags_part_text.split('\n'):
                if line.find('@Input') != -1:
                    input_list.append(line.split(' ')[1].replace('\'', ''))
                elif line.find('@Output') != -1:
                    output_list.append(line.split(' ')[1].replace('\'', ''))
                elif line.find('@EtlURL') != -1:
                    etl_list.append(line.split(' ')[1].replace('\'', ''))
                elif line.find('@ReactorURL') != -1:
                    reactor_list.append(line.split(' ')[1].replace('\'', ''))
                elif line.find('@Owner') != -1:
                    owner_list.append(line.split(' ')[1].replace('\'', ''))
                else:
                    continue
            t = Tags(input=input_list, etl=etl_list, reactor=reactor_list, owner=owner_list)
            for output in list(set(output_list)):
                if d.get(output, None) is not None:
                    if isinstance(d[output], list):
                        d[output] = concat_lists(d[output], [t])
                    else:
                        d[output] = [d[output], t]
                else:
                    d[output] = t
    return d


def main():
    pprint(parse_tags(receive_all_sql_files(PATH)))


if __name__ == "__main__":
    main()
