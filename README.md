# jdbi-examples
Code examples for getting familiar with the JDBI API

## Running tests from IntelliJ

Some of the v3 examples depend on the `-parameters` compiler flag being
set. This is configured in `pom.xml`, but unfortunately IntelliJ doesn't
pick it up.

* Open IntelliJ Preferences
* Navigate to Build, Execution, Deployment -> Compiler -> Java Compiler
* Under JavaC Options -> Additional command line parameters, add `-parameters`.
