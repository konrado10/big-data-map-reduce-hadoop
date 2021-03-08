from mrjob.job import MRJob

# Aby uruchomić job'a należy użyć komendy:
# $ python nazwa_skyptu.py nazwa_pliku_wejściowego.txt
# $ python zad1.py data.txt data2.txt

class MRWordCount(MRJob):

    def mapper(self, _, line):
        yield 'chars', len(line)
        yield 'words', len(line.split())

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordCount.run()