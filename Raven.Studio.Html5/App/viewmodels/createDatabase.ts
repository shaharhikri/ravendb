import app = require("durandal/app");
import document = require("models/document");
import dialog = require("plugins/dialog");
import createDatabaseCommand = require("commands/createDatabaseCommand");
import collection = require("models/collection");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import createEncryption = require("viewmodels/createEncryption");

class createDatabase extends dialogViewModelBase {

    public creationTask = $.Deferred();
    creationTaskStarted = false;

    databaseName = ko.observable('');
    isCompressionBundleEnabled = ko.observable(false);
    isEncryptionBundleEnabled = ko.observable(false);
    isExpirationBundleEnabled = ko.observable(false);
    isQuotasBundleEnabled = ko.observable(false);
    isReplicationBundleEnabled = ko.observable(false);
    isSqlReplicationBundleEnabled = ko.observable(false);
    isVersioningBundleEnabled = ko.observable(false);
    isPeriodicBackupBundleEnabled = ko.observable(true); // Old Raven Studio has this enabled by default
    isScriptedIndexBundleEnabled = ko.observable(false);

    private encryptionData = new Object();

    constructor() {
        super();
    }

    cancel() {
        dialog.close(this);
    }

    deactivate() {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never started it.
        if (!this.creationTaskStarted) {
            this.creationTask.reject();
        }
    }

    nextOrCreate() {
        // Next needs to configure bundle settings, if we've selected some bundles.
        // We haven't yet implemented bundle configuration, so for now we're just 
        // creating the database.
        /*var self = this;
        var databaseName = this.databaseName();

        new createDatabaseCommand(databaseName, self.getActiveBundles(), this.encryptionData)
            .execute()
            .fail(response=> {
                self.creationTask.reject(response);
            })
            .done(result=> {
                self.creationTask.resolve(databaseName);
                self.creationTaskStarted = true;
                dialog.close(self);
            });*/

        var databaseName = this.databaseName();
        this.creationTaskStarted = true;
        debugger;
        dialog.close(this, { databaseName: databaseName, bundles: this.getActiveBundles() });
    }



    toggleCompressionBundle() {
        this.isCompressionBundleEnabled.toggle();
    }

    toggleEncryptionBundle() {
        this.isEncryptionBundleEnabled.toggle();
        /*if (self.isEncryptionBundleEnabled() == true) {
            var createEncryptionViewModel: createEncryption = new createEncryption();
            createEncryptionViewModel
                .creationEncryption
                .fail(
                dialogResult => {
                    self.isEncryptionBundleEnabled.toggle();
                    self.encryptionData = null;
                    $('#encryptionCheckbox').removeClass('active');
                })
                .done((keyName: string, encryptionAlgorithm: string, isEncryptedIndexes: string) => {
                    var encriptionSettings: string[] = [];
                    encriptionSettings.push(keyName, encryptionAlgorithm, isEncryptedIndexes);

                    self.encryptionData = {
                        'Raven/Encryption/Key': keyName,
                        'Raven/Encryption/Algorithm': this.getEncryptionAlgorithmFullName(encryptionAlgorithm),
                        'Raven/Encryption/EncryptIndexes': isEncryptedIndexes
                    };
                });

            app.showDialog(createEncryptionViewModel);
        }*/
    }

    toggleExpirationBundle() {
        this.isExpirationBundleEnabled.toggle();
    }

    toggleQuotasBundle() {
        this.isQuotasBundleEnabled.toggle();
    }

    toggleReplicationBundle() {
        this.isReplicationBundleEnabled.toggle();
    }

    toggleSqlReplicationBundle() {
        this.isSqlReplicationBundleEnabled.toggle();
    }

    toggleVersioningBundle() {
        this.isVersioningBundleEnabled.toggle();
    }

    togglePeriodicBackupBundle() {
        this.isPeriodicBackupBundleEnabled.toggle();
    }

    toggleScriptedIndexBundle() {
        this.isScriptedIndexBundleEnabled.toggle();
    }

    private getActiveBundles(): string[] {
        var activeBundles: string[] = [];
        if (this.isCompressionBundleEnabled()) {
            activeBundles.push("Compression");
        }

        if (this.isEncryptionBundleEnabled()) {
            activeBundles.push("Encryption"); // TODO: Encryption also needs to specify 2 additional settings: http://ravendb.net/docs/2.5/server/extending/bundles/encryption?version=2.5
        }

        if (this.isExpirationBundleEnabled()) {
            activeBundles.push("DocumentExpiration");
        }

        if (this.isQuotasBundleEnabled()) {
            activeBundles.push("Quotas");
        }

        if (this.isReplicationBundleEnabled()) {
            activeBundles.push("Replication"); // TODO: Replication also needs to store 2 documents containing information about replication. See http://ravendb.net/docs/2.5/server/scaling-out/replication?version=2.5
        }

        if (this.isSqlReplicationBundleEnabled()) {
            activeBundles.push("SqlReplication");
        }

        if (this.isVersioningBundleEnabled()) {
            activeBundles.push("Versioning");
        }

        if (this.isPeriodicBackupBundleEnabled()) {
            activeBundles.push("PeriodicBackups");
        }

        if (this.isScriptedIndexBundleEnabled()) {
            activeBundles.push("ScriptedIndexResults");
        }
        return activeBundles;
    }
}

export = createDatabase;