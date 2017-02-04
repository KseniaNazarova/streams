package part1.exercise;

import data.Employee;
import data.JobHistoryEntry;
import data.Person;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;
import static org.junit.Assert.assertEquals;

public class StreamsExercise2 {
    // https://youtu.be/kxgo7Y4cdA8 Сергей Куксенко и Алексей Шипилёв — Через тернии к лямбдам, часть 1
    // https://youtu.be/JRBWBJ6S4aU Сергей Куксенко и Алексей Шипилёв — Через тернии к лямбдам, часть 2

    // https://youtu.be/O8oN4KSZEXE Сергей Куксенко — Stream API, часть 1
    // https://youtu.be/i0Jr2l3jrDA Сергей Куксенко — Stream API, часть 2

    private static class PersonEmployerPair{
        private Person person;
        private String employer;

        public PersonEmployerPair(Person person, String employer) {
            this.person = person;
            this.employer = employer;
        }

        public Person getPerson() {
            return person;
        }

        public String getEmployer() {
            return employer;
        }
    }

    private Stream<PersonEmployerPair> getPersonEmployerPairs(Employee employee){
        return employee.getJobHistory()
                .stream()
                .map(JobHistoryEntry::getEmployer)
                .map(e -> new PersonEmployerPair(employee.getPerson(), e));
    }

    private Stream<PersonEmployerPair> getPersonFirstEmployerPairs(Employee employee){
        return employee.getJobHistory()
                .stream()
                .map(JobHistoryEntry::getEmployer)
                .limit(1)
                .map(e -> new PersonEmployerPair(employee.getPerson(), e));
    }

    @Test
    public void employersStuffLists() {
        List<Employee> employees = getEmployees();
        Stream<PersonEmployerPair> personEmployerPairStream = employees
                .stream()
                .flatMap(this::getPersonEmployerPairs);

        Map<String, Set<Person>> actual = personEmployerPairStream
                .collect(Collectors.groupingBy(
                                PersonEmployerPair::getEmployer,
                                Collectors.mapping(PersonEmployerPair::getPerson, toSet())));

        Map<String, Set<Person>> expected = new HashMap<>();

        expected.put("epam", new HashSet<>(Arrays.asList(
                new Person("John", "Galt", 20),
                new Person("John", "Doe", 21),
                new Person("John", "White", 22),
                new Person("John", "Galt", 23),
                new Person("John", "Doe", 24),
                new Person("John", "White", 25),
                new Person("John", "Galt", 26),
                new Person("Bob", "Doe", 27),
                new Person("John", "White", 28),
                new Person("John", "Galt", 29),
                new Person("John", "Doe", 30),
                new Person("Bob", "White", 31))));

        expected.put("abc", new HashSet<>(Arrays.asList(
                new Person("John", "Doe", 21),
                new Person("Bob", "Doe", 27),
                new Person("John", "Doe", 30),
                new Person("John", "Doe", 24))));

        expected.put("yandex", new HashSet<>(Arrays.asList(
                new Person("John", "Doe", 21),
                new Person("Bob", "Doe", 27),
                new Person("John", "Doe", 30),
                new Person("John", "Doe", 24))));

        expected.put("google", new HashSet<>(Arrays.asList(
                new Person("John", "Galt", 29),
                new Person("John", "Galt", 26),
                new Person("John", "Galt", 23),
                new Person("John", "Galt", 20)

        )));

        assertEquals(expected, actual);
    }

    @Test
    public void indexByFirstEmployer() {

        Stream<PersonEmployerPair> personEmployerPairs = getEmployees()
                .stream()
                .flatMap(this::getPersonFirstEmployerPairs);

        Map<String, Set<Person>> actual = personEmployerPairs.collect(Collectors.groupingBy(
                PersonEmployerPair::getEmployer,
                Collectors.mapping(PersonEmployerPair::getPerson, toSet())));

        Map<String, Set<Person>> expected = new HashMap<>();

        expected.put("epam", new HashSet<>(Arrays.asList(
                new Person("John", "Galt", 20),
                new Person("John", "White", 22),
                new Person("John", "Galt", 23),
                new Person("John", "White", 25),
                new Person("John", "Galt", 26),
                new Person("John", "White", 28),
                new Person("John", "Galt", 29),
                new Person("Bob", "White", 31))));

        expected.put("yandex", new HashSet<>(Arrays.asList(
                new Person("John", "Doe", 21),
                new Person("John", "Doe", 24),
                new Person("Bob", "Doe", 27),
                new Person("John", "Doe", 30))));

        assertEquals(expected, actual);
    }

    private static class PersonEmployerDuration {
        private final Person person;
        private final String employer;
        private final int duration;

        public PersonEmployerDuration(Person person, String employer, int duration) {
            this.person = person;
            this.employer = employer;
            this.duration = duration;
        }

        public Person getPerson() {
            return person;
        }

        public String getEmployer() {
            return employer;
        }

        public int getDuration() {
            return duration;
        }
    }

    @Test
    public void greatestExperiencePerEmployer() {

        List<Employee> employees = getEmployees();

        Stream<PersonEmployerDuration> personEmployerDurationStream = employees.stream()
                .flatMap(e -> e.getJobHistory()
                        .stream()
                        .map(j -> new PersonEmployerDuration(e.getPerson(), j.getEmployer(), j.getDuration())));

        Map<String, Person> employeesIndex = personEmployerDurationStream.collect(
                groupingBy(
                        PersonEmployerDuration::getEmployer,
                        collectingAndThen(
                                maxBy(Comparator.comparing(PersonEmployerDuration::getDuration)),
                                p -> p.get().getPerson())));

        assertEquals(new Person("John", "White", 28), employeesIndex.get("epam"));
    }

    private List<Employee> getEmployees() {
        return Arrays.asList(
                new Employee(
                        new Person("John", "Galt", 20),
                        Arrays.asList(
                                new JobHistoryEntry(3, "dev", "epam"),
                                new JobHistoryEntry(2, "dev", "google")
                        )),
                new Employee(
                        new Person("John", "Doe", 21),
                        Arrays.asList(
                                new JobHistoryEntry(4, "BA", "yandex"),
                                new JobHistoryEntry(2, "QA", "epam"),
                                new JobHistoryEntry(2, "dev", "abc")
                        )),
                new Employee(
                        new Person("John", "White", 22),
                        Collections.singletonList(
                                new JobHistoryEntry(6, "QA", "epam")
                        )),
                new Employee(
                        new Person("John", "Galt", 23),
                        Arrays.asList(
                                new JobHistoryEntry(3, "dev", "epam"),
                                new JobHistoryEntry(2, "dev", "google")
                        )),
                new Employee(
                        new Person("John", "Doe", 24),
                        Arrays.asList(
                                new JobHistoryEntry(4, "QA", "yandex"),
                                new JobHistoryEntry(2, "BA", "epam"),
                                new JobHistoryEntry(2, "dev", "abc")
                        )),
                new Employee(
                        new Person("John", "White", 25),
                        Collections.singletonList(
                                new JobHistoryEntry(6, "QA", "epam")
                        )),
                new Employee(
                        new Person("John", "Galt", 26),
                        Arrays.asList(
                                new JobHistoryEntry(3, "dev", "epam"),
                                new JobHistoryEntry(1, "dev", "google")
                        )),
                new Employee(
                        new Person("Bob", "Doe", 27),
                        Arrays.asList(
                                new JobHistoryEntry(4, "QA", "yandex"),
                                new JobHistoryEntry(2, "QA", "epam"),
                                new JobHistoryEntry(2, "dev", "abc")
                        )),
                new Employee(
                        new Person("John", "White", 28),
                        Collections.singletonList(
                                new JobHistoryEntry(666, "BA", "epam")
                        )),
                new Employee(
                        new Person("John", "Galt", 29),
                        Arrays.asList(
                                new JobHistoryEntry(3, "dev", "epam"),
                                new JobHistoryEntry(1, "dev", "google")
                        )),
                new Employee(
                        new Person("John", "Doe", 30),
                        Arrays.asList(
                                new JobHistoryEntry(4, "QA", "yandex"),
                                new JobHistoryEntry(2, "QA", "epam"),
                                new JobHistoryEntry(5, "dev", "abc")
                        )),
                new Employee(
                        new Person("Bob", "White", 31),
                        Collections.singletonList(
                                new JobHistoryEntry(6, "QA", "epam")
                        ))
        );
    }

}
