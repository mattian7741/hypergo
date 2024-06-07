def pass_message(body):
    # pipeline orchestrator has one job: passing a message from one component to the next by updating its routing key.
    # Thus, it doesn't care what the body of the input/output is, and the
    # function itself is basically a no-op
    return body
