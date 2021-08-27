import datetime

from influxdb_client import DashboardsService, CreateDashboardRequest, CellsService, \
    CreateCell
from tests.base_test import BaseTest


class DashboardsClientTest(BaseTest):

    def setUp(self) -> None:
        super(DashboardsClientTest, self).setUp()

        self.dashboards_service = DashboardsService(self.client.api_client)
        dashboards = self.dashboards_service.get_dashboards()

        for dashboard in dashboards.dashboards:
            if dashboard.name.endswith("_IT"):
                print("Delete dashboard: ", dashboard.name)
                self.dashboards_service.delete_dashboards_id(dashboard.id)

    def test_create_dashboard_with_cell(self):
        unique_id = str(datetime.datetime.now().timestamp())

        dashboard = self.dashboards_service.post_dashboards(
            create_dashboard_request=CreateDashboardRequest(org_id=self.find_my_org().id, name=f"Dashboard_{unique_id}_IT"))
        self.assertEqual(dashboard.name, f"Dashboard_{unique_id}_IT")

        cells_service = CellsService(self.client.api_client)
        cell = cells_service.post_dashboards_id_cells(
            dashboard_id=dashboard.id, create_cell=CreateCell(name=f"Cell_{unique_id}_IT", h=3, w=12))
        self.assertIsNotNone(cell.id)
        view = cells_service.get_dashboards_id_cells_id_view(dashboard_id=dashboard.id, cell_id=cell.id)
        self.assertEqual(view.name, f"Cell_{unique_id}_IT")

    def test_get_dashboard_with_cell_with_properties(self):
        unique_id = str(datetime.datetime.now().timestamp())

        dashboard = self.dashboards_service.post_dashboards(
            create_dashboard_request=CreateDashboardRequest(org_id=self.find_my_org().id,
                                                            name=f"Dashboard_{unique_id}_IT"))

        # create cell
        CellsService(self.client.api_client).post_dashboards_id_cells(
            dashboard_id=dashboard.id, create_cell=CreateCell(name=f"Cell_{unique_id}_IT", h=3, w=12))

        # retrieve dashboard
        dashboard = self.dashboards_service.get_dashboards_id(dashboard.id)

        from influxdb_client import DashboardWithViewProperties, CellWithViewProperties
        self.assertEqual(DashboardWithViewProperties, type(dashboard))
        self.assertEqual(1, len(dashboard.cells))
        self.assertEqual(CellWithViewProperties, type(dashboard.cells[0]))
