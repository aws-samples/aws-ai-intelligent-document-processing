class LinearizeLayout:
    def __init__(self, j: dict):
        self.j = j
        
    def _get_layout_blocks(self) -> tuple:
        """Get all blocks of type 'LAYOUT' and a dictionary of Ids mapped to their corresponding block."""
        layouts = [x for x in self.j['Blocks'] if x['BlockType'].startswith('LAYOUT')]
        id2block = {x['Id']: x for x in self.j['Blocks']}
        return layouts, id2block


    def _dfs(self,root,id2block):
        texts = []
        stack = [(root,0)]
        while stack:
            block_id,depth = stack.pop()
            block = id2block[block_id]
            if block["BlockType"] == "LINE" and "Text" in block:
                texts += block['Text'],       
            if block["BlockType"].startswith('LAYOUT'):
                if "Relationships" in block:
                    relationships = block["Relationships"]
                    assert len(relationships) == 1
                    assert relationships[0]['Type'] == "CHILD"            
                    children = [(x,depth+1) for x in relationships[0]['Ids']]            
                    stack.extend(reversed(children))
        return texts
    
    def get_text(self) -> str:
        texts = []
        layouts, id2block = self._get_layout_blocks()
        for layout in layouts:
            root = layout['Id']
            texts += '\n'.join(self._dfs(root,id2block)),
        text = '\n\n'.join(texts)
        return text