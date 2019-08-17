version 1.0

workflow SayHello {

    input {
        String name = "World"
    }

    call Hello {
        input:
            name = name
    }

    output {
        String msg = Hello.msg
    }
}

task Hello {

  input {
    String name
  }

  command {
    echo Hello ~{name}!
  }

  runtime {
    docker: "debian:10-slim"
  }

  output {
    String msg = read_string(stdout())
  }
}
