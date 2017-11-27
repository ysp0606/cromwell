id: first_tool_id
cwlVersion: v1.0
class: CommandLineTool
baseCommand: echo
inputs:
  message:
    type: string
    inputBinding:
      position: 1
stdout: echo-stdOut.txt
outputs:
- id: out
  outputBinding:
    glob: echo-stdOut.txt
    loadContents: true
    outputEval: $(self[0].contents)
  type: string
