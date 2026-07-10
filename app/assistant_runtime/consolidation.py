from dataclasses import dataclass
from typing import List
@dataclass
class Candidate: id:str; status:str; section:str; content:str
class ConsolidationEngine:
    def run(self,candidates:List[Candidate],model:dict):
        report=[]
        for c in candidates:
            if c.status!="APPROVED": continue
            model.setdefault(c.section,[]).append(c.content)
            report.append({"id":c.id,"section":c.section})
        return {"status":"completed","report":report,"readback":model}
