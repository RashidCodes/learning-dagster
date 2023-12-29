from dagster import ConfigurableResource 

class SolverClientResource(ConfigurableResource):
    user_id: str 

    def extract_solver_data(self):
        return self.user_id