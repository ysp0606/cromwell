cwlVersion: v1.0
class: Workflow
id: cwl_outputs
outputs:
- id: outputs-hello
  outputSource: "#echo/out"
steps:
- id: echo
  in: []
  out:
  - id: ps-stdOut
  run:
    inputs: []
    outputs:
    - id: out
      outputBinding:
        glob: echo-stdOut.txt
      type: String
    class: CommandLineTool
    arguments:
    - valueFrom: echo
      shellQuote: false
    - valueFrom: grep
      shellQuote: false
    stdout: echo-stdOut.txt
    requirements:
    - class: ShellCommandRequirement
