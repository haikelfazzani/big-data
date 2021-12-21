# Created by Haikel Fazzani

from mrjob.job import MRJob
from mrjob.step import MRStep

# oldest survived Male and Female
# Result should be: [74, 0] [80, 1]

def num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)

class oldestSurvived(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_survived,
                   reducer=self.reducer_count_survived),
            # MRStep(mapper=self.reducer_count_unsurvived, reducer = self.reducer_count_unsurvived)
        ]

    def mapper_survived(self, _, line):
        (PassengerId, Survived, Pclass, FirstName, SecondName, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked) = line.split(',')
        if len(Age) > 0:
            yield int(Survived), int(float(Age.replace(",", ".")))

    def reducer_count_survived(self, key, values):
        yield None, (max(values), key)

if __name__ == '__main__':
    oldestSurvived.run()