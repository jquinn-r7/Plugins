import csv
import gc
import io
from datetime import datetime
from flask import request, stream_with_context
from werkzeug.datastructures import Headers
from werkzeug.wrappers import Response

from DivvyBlueprints.v2 import Blueprint
from DivvyDb import DivvyDbObjects
from DivvyDb.DivvyCloudGatewayORM import DivvyCloudGatewayORM
from DivvyDb.DivvyDb import SharedSessionScope
from DivvyInterfaceMessages import v2_interface_protocol
from DivvyPermissions.RolePermissions import RolePermissions
from DivvyPlugins.plugin_helpers import (
    register_api_blueprint, unregister_api_blueprints
)
from DivvyResource import Resources
from DivvySession import DivvySession
from DivvyUtils.flask_helpers import JsonResponse

blueprint = Blueprint('resourceinventory', __name__)

@blueprint.route('/inventory', methods=['POST'])
def get_resource_inventory():
    """
    This blueprint leverages the user's session object to reference their last
    resource "view". The view is stored within their session and includes key
    information such as the scope, and filters.
    """

    session = DivvySession.current_session()
    db = DivvyCloudGatewayORM()
    data = request.get_json() or {}
    scopes = data.get('scopes')
    resource_types = data.get('resource_types')
    badges = data.get('badges')
    badge_filter_operator = data.get('badge_filter_operator', 'OR')

    # Scopes query to pull out the organization service IDs that the user
    # has permission to.
    scopes_query = db.session.query(
        DivvyDbObjects.OrganizationService.organization_service_id,
    ).filter(
        DivvyDbObjects.OrganizationService.organization_id ==
        session.user.organization_id
    )

    # If we have more than one organization then we need to filter on
    # organization services within the calling user's organization. Otherwise
    # we can default to None.
    if scopes:
        scopes_query = scopes_query.filter(
            DivvyDbObjects.OrganizationService.resource_id.in_(scopes)
        )

    elif badges:
        # Convert badges to badge objects
        badge_objects = []
        for badge in badges:
            badge_objects.append(
                v2_interface_protocol.DivvyBadgeInfo(
                    key=badge['key'],
                    value=badge['value']
                )
            )
        scopes_query = Resources.ResourceGroup_OrganizationService._filter_badges(
            session.user.organization_id,
            query=scopes_query,
            badges=badge_objects,
            operator=badge_filter_operator
        )

    # Pass the query through RolePermissions to scope it down to only
    # resources the user has access to.
    scopes_query = RolePermissions.filter_resource_query(
        db_session=db.session,
        query=scopes_query,
        user=session.user,
        db_class=DivvyDbObjects.OrganizationService,
        permissions=(RolePermissions.VIEW,)
    )
    scopes = [row.organization_service_id for row in scopes_query]

    @SharedSessionScope(DivvyCloudGatewayORM)
    def get_resources(
        session, organization_id, user, scopes, resource_types, limit, offset
    ):
        query = session.query(
            DivvyDbObjects.ResourceCommonData.provider_id,
            DivvyDbObjects.ResourceCommonData.name,
            DivvyDbObjects.ResourceCommonData.region_name,
            DivvyDbObjects.ResourceCommonData.resource_type,
            DivvyDbObjects.ResourceMatrix.name.label('cloud_resource_type'),
            DivvyDbObjects.ResourceCommonData.namespace_id,
            DivvyDbObjects.ResourceCommonData.creation_timestamp,
            DivvyDbObjects.ResourceCommonData.discovered_timestamp,
            DivvyDbObjects.OrganizationService.account_id,
            DivvyDbObjects.OrganizationService.name.label('account'),
            DivvyDbObjects.OrganizationService.cloud_type_id
        ).filter(
            DivvyDbObjects.ResourceCommonData.organization_service_id == DivvyDbObjects.OrganizationService.organization_service_id,
            DivvyDbObjects.OrganizationService.organization_id == organization_id,
            DivvyDbObjects.OrganizationService.cloud_type_id == DivvyDbObjects.ResourceMatrix.cloud_type_id,
            DivvyDbObjects.ResourceCommonData.resource_type == DivvyDbObjects.ResourceMatrix.resource_type,
        )

        if resource_types:
            query = query.filter(
                DivvyDbObjects.ResourceCommonData.resource_type.in_(resource_types)
            )

        if scopes:
            query = query.filter(
                DivvyDbObjects.OrganizationService.organization_service_id.in_(scopes)
            )

        if limit:
            query = query.limit(limit)

        if offset:
            query = query.offset(offset)

        return query

    @SharedSessionScope(DivvyCloudGatewayORM)
    def generate(session, scopes, resource_types):
        PAGE_SIZE = 10000
        db = DivvyCloudGatewayORM()

        headers = [
            'ID', 'Name', 'Region', 'Resource Type', 'Cloud Resource Type',
            'Cloud Account', 'Account ID', 'Namespace ID',
            'Discovered Timestamp', 'Creation Timestamp'
        ]
        data = io.StringIO()
        data.truncate(0)
        writer = csv.writer(data)
        writer.writerow(headers)
        yield data.getvalue()

        data.seek(0)
        data.truncate(0)

        resources = get_resources(
            session=db.session,
            organization_id=session.user.organization_id,
            user=session.get_user().get_db_object(),
            scopes=scopes,
            resource_types=resource_types,
            limit=PAGE_SIZE,
            offset=None
        )

        offset = PAGE_SIZE
        do_it = True
        while do_it:
            items_done = 0
            data.seek(0)
            data.truncate(0)
            for resource in resources:
                writer.writerow([
                    resource.provider_id,
                    resource.name,
                    resource.region_name,
                    resource.resource_type,
                    resource.cloud_resource_type,
                    resource.account,
                    resource.account_id,
                    resource.namespace_id,
                    resource.discovered_timestamp,
                    resource.creation_timestamp
                ])
                yield data.getvalue()
                data.seek(0)
                data.truncate(0)
                items_done += 1

            del resources
            gc.collect()

            resources = get_resources(
                session=db.session,
                organization_id=session.user.organization_id,
                user=session.get_user().get_db_object(),
                scopes=scopes,
                resource_types=resource_types,
                limit=PAGE_SIZE,
                offset=offset
            )
            offset += PAGE_SIZE
            if resources is None or items_done < PAGE_SIZE:
                do_it = False


    # Build headers
    headers = Headers()
    headers.set('Content-Disposition', 'attachment', filename='resourceinventory-{0}.csv'.format(
        datetime.strftime(datetime.now(), '%Y.%m.%d')
    ))

    # Stream the response as the data is generated
    return Response(
        stream_with_context(generate(session, scopes, resource_types)),
        mimetype='text/csv', headers=headers
    )


def load():
    register_api_blueprint(blueprint)

def unload():
    unregister_api_blueprints()
