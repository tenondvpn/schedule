import logging
from django.db import models
from django.conf import settings
from django.db.models import F
logger = logging.getLogger(settings.PROJECT_NAME)
from django.contrib.auth.models import User

PERM_TYPE_READ = 'read'
PERM_TYPE_UPDATE = 'update'
PERM_TYPE_DELETE = 'delete'
PERM_TYPE_EXECUTE = 'execute'
PERM_TYPES = (PERM_TYPE_READ, PERM_TYPE_UPDATE,
              PERM_TYPE_DELETE, PERM_TYPE_EXECUTE)
PERM_SEP = '|'


def parse_permission(permission):
    perms = {}
    if isinstance(permission, str):
        for perm in permission.split(PERM_SEP):
            if perm in PERM_TYPES:
                perms[perm] = True
    return perms


def make_permission(read=None, update=None,
                    delete=None, execute=None):
    perms = ((PERM_TYPE_READ, read),
             (PERM_TYPE_UPDATE, update),
             (PERM_TYPE_DELETE, delete),
             (PERM_TYPE_EXECUTE, execute))
    perm_list = []
    for perm, value in perms:
        if True == value:
            perm_list.append(perm)
    return PERM_SEP.join(perm_list)

class CF(F):
    ADD = '||'

class PermHistoryManager(models.Manager):

    def __init__(self, pending_status, accepted_status, denied_status,
                 granted_status, revoked_status, *args, **kwargs):

        super(PermHistoryManager, self).__init__(*args, **kwargs)
        self.pending_status = pending_status
        self.accepted_status = accepted_status
        self.denied_status = denied_status
        self.granted_status = granted_status
        self.revoked_status = revoked_status

    # recipient: id of users who the permission is granted to
    def grant(self, grantor, recipient, resource_type, resource_id, reason = '',
              read = True, update = None, delete = None, execute = None, 
              permission = ''):
        
        if not permission:
            permission = make_permission(read, update, delete, execute)
        if not isinstance(recipient, list):
            recipient = [recipient]
        for user in recipient:
            try:
                self.create(
                    resource_type = resource_type, resource_id = resource_id,
                    permission = permission, grantor = grantor, applicant = user,
                    reason = reason, status = self.granted_status)
            except Exception as ex:
                logger.error('grant permission fail: (%s)' % str(ex))
                raise Exception('grant permission fail')


    # apply for permissions
    def apply(self, applicant, grantor, resource_type, resource_id, reason = '',
              read = True, update = None, delete = None, execute = None,
              permission = ''):
        
        if not permission:
            permission = make_permission(read, update, delete, execute)
        try:
            item, created = self.update_or_create(
                resource_type = resource_type, resource_id = resource_id,
                grantor = grantor, applicant = applicant, 
                status = self.pending_status,
                defaults = {'permission': permission, 'reason': reason})
            return item
        except Exception as ex:
            logger.error('apply permission fail: (%s)' % str(ex))
            raise Exception('apply permission fail')


    # from_user: id of users who the permission is revoked from
    def revoke(self, revoker, from_user, resource_type, resource_id, reason = '',
               read = None, update = None, delete = None, execute = None,
               permission = '', odps_perm_type=-1):

        if not permission:
            permission = make_permission(read, update, delete, execute)
        if not isinstance(from_user, list):
            from_user = [from_user]
        for user in from_user:
            try:
                item = self.create(
                    resource_type = resource_type, resource_id = resource_id,
                    permission = permission, grantor = revoker, 
                    applicant = user, reason = reason, 
                    status = self.revoked_status,
                    odps_perm_type=odps_perm_type)
                return item
            except Exception as ex:
                logger.error('revoke permission fail: (%s)' % str(ex))
                raise Exception('revoke permission fail')
        
        
    # approve permission application
    def accept(self, hist_id, reason = ''):
        
        try:
            item = self.get(id = hist_id)
            item.status = self.accepted_status
            item.reason = item.reason + '\naccept:' + reason
            item.save()
            return item
        except Exception as ex:
            logger.error('accept permission fail: (%s)' % str(ex))
            raise Exception('accept permission fail')


    # deny permission application
    def deny(self, hist_id, reason = ''):
        try:
            item = self.get(id = hist_id)
            item.status = self.denied_status
            item.reason = item.reason + '\nreject:' + reason
            item.save()
            return item
        except Exception as ex:
            logger.error('deny permission fail: (%s)' % str(ex))
            raise Exception('deny permission fail')


    # the apply list
    def get_pending_list(self, resource_type, resource_id, grantor = None):
        return self.list(
            resource_type, resource_id, grantor, pending_only = True)


    # list perm_history
    def list(self, resource_type, resource_id, grantor = None,
             pending_only = False):
        objects = self.select_related('proposer_user')
        if grantor and isinstance(grantor, User):
            objects = objects.filter(grantor = grantor)
        if resource_type and resource_id:
            objects = objects.filter(
                resource_type = resource_type, 
                resource_id = resource_id)
        if pending_only:
            objects = objects.filter(status = self.pending_status)
        return objects

