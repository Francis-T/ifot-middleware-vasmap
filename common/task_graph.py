import json

class TaskGraph():
    def __init__(self, unique_id):
        self.ref_id = 0
        self.order = 0
        self.unique_id = unique_id
        self.task_graph = []
        self.exec_task_funcs = {}
        self.add_task_funcs = {}

        return

    def add_task_type(self, task_type, exec_func, add_func):
        if task_type == None or exec_func == None or add_func == None:
            return False

        self.exec_task_funcs[task_type] = str(exec_func)
        self.add_task_funcs[task_type] = add_func
        return True

    def create_task(self, node_id, task_type, task_func, extras=None, seq_id=1):
        task = {
            'ref_id'        : self.ref_id,
            'unique_id'     : self.unique_id,
            'type'          : task_type,
            'seq_id'        : seq_id,
            'func'          : task_func,
            'order'         : self.order,
            'node_id'       : node_id,
            'dest'          : [], 
        }

        if extras:
            for k in list(extras.keys()):
                task[k] = extras[k]

        return task

    def add_tasks(self, node_id_list, task_type, extras=None):
        # Resolve the function to be used for adding tasks
        if not task_type in self.add_task_funcs:
            print("ERROR Invalid task_type: {}".format(task_type))
            return []

        add_task_func = self.add_task_funcs[task_type]

        added_tasks = []
        seq_id = 1
        for node_id in node_id_list:
            added_tasks.append( add_task_func(node_id, seq_id, extras=extras) )
            seq_id += 1

        self.order += 1
        return added_tasks

    def add_next_dest(self, task, target_list):
        # Get the distinct combination of node ids and task types
        dest_tasks = task['dest']
        for target in target_list:
            already_exists = False
            for dest in dest_tasks:
                # Check if each target already exists in the dest_tasks list
                if dest['type'] == target['type'] and \
                   dest['func'] == target['func']:
                    
                    # Add the new target to the dest_tasks list of nodes
                    #  if it does not yet exist
                    if not target['node_id'] in dest['nodes']:
                        dest['nodes'].append( target['node_id'] )
                        already_exists = True
           
            if already_exists == True:
                continue

            # If it doesn't, create a new entry for it
            new_dest = {
                'nodes' : [ target['node_id'] ],
                'type'  : target['type'],
                'func'  : target['func'],
                'order' : task['order'] + 1,
            }
            dest_tasks.append( new_dest )

        return

    def get_task_graph(self):
        return self.task_graph

    def __repr__(self):
        return json.dumps(self.task_graph, indent=4)

