from mrjob.job import MRJob
from mrjob.step import MRStep

class NumberOfMaleSurvived(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_survived,
                   reducer=self.reducer_count_survived),
            # MRStep(mapper=self.reducer_count_unsurvived, reducer = self.reducer_count_unsurvived)
        ]

    def mapper_survived(self, _, line):
        (PassengerId, Survived, Pclass, FirstName, SecondName, Sex, Age, SibSp, Parch, Ticket, Fare, Cabin, Embarked) = line.split(',')
        if int(Survived) > 0 and Sex == 'male':
            yield Survived, 1    

    def reducer_count_survived(self, key, values):
        yield None, (sum(values), key)

    # def mapper_unsurvived(self, _, line):
    #     (PassengerId, Survived, Pclass, FirstName, SecondName,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked) = line.split(',')
    #     if int(Survived) < 1:
    #         yield Survived, 1

    # def reducer_count_unsurvived(self, key, values):
    #     yield max(values)

if __name__ == '__main__':
    NumberOfMaleSurvived.run()