package org.appian.community.azurefileshare;

import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.content.ContentConstants;
import com.appiancorp.suiteapi.content.ContentService;
import com.appiancorp.suiteapi.knowledge.Document;
import com.appiancorp.suiteapi.process.exceptions.SmartServiceException;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.framework.SmartServiceContext;
import com.appiancorp.suiteapi.process.palette.PaletteCategoryConstants;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.appiancorp.suiteapi.security.external.SecureCredentialsStore;

import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareClientBuilder;
import com.azure.storage.file.share.ShareDirectoryClient;
import com.azure.storage.file.share.ShareFileClient;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

@PaletteInfo(paletteCategory = PaletteCategoryConstants.APPIAN_SMART_SERVICES, palette = "Connectivity Services")
public class AzureLargeFileUploader extends AppianSmartService {

    private static final Logger LOG = Logger.getLogger(AzureLargeFileUploader.class);

    private final ContentService contentService;
    private final SecureCredentialsStore scs;
    private final SmartServiceContext smartServiceCtx;

    // Inputs
    private String accountUrl;
    private String shareName;
    private String directoryPath;
    private String fileName;
    private Long appianDocumentId;
    private String manualSasToken;
    private String scsKey;
    private String scsField;

    // Outputs
    private Boolean isSuccess;
    private String errorMessage;

    public AzureLargeFileUploader(ContentService contentService, SecureCredentialsStore scs, SmartServiceContext smartServiceCtx) {
        this.contentService = contentService;
        this.scs = scs;
        this.smartServiceCtx = smartServiceCtx;
    }

    @Override
    public void run() throws SmartServiceException {
        InputStream inputStream = null;
        try {
            // 1. Resolve Authentication Token
            String sasToken = manualSasToken;
            if (sasToken == null || sasToken.trim().isEmpty()) {
                if (scsKey != null && scsField != null) {
                    Map<String, String> credentials = scs.getSystemSecuredValues(scsKey);
                    if (credentials != null && credentials.containsKey(scsField)) {
                        sasToken = credentials.get(scsField);
                    }
                }
            }

            if (sasToken == null || sasToken.trim().isEmpty()) {
                throw new IllegalArgumentException("No SAS Token provided via SCS or Manual Input.");
            }

            // 2. Build Azure Client
            String endpoint = accountUrl + "/" + shareName + "?" + sasToken;
            ShareClient shareClient = new ShareClientBuilder()
                    .endpoint(endpoint)
                    .buildClient();

            ShareDirectoryClient dirClient = shareClient.getRootDirectoryClient();

            // 3. Normalize and walk the directory path
            if (directoryPath != null && !directoryPath.trim().isEmpty()) {
                String normalized = directoryPath.replace('\\', '/').trim();
                for (String folder : normalized.split("/")) {
                    if (!folder.isEmpty()) {
                        dirClient = dirClient.getSubdirectoryClient(folder);
                        if (!dirClient.exists()) {
                            dirClient.create();
                        }
                    }
                }
            }

            // 4. File Client Setup & Idempotency Check
            ShareFileClient fileClient = dirClient.getFileClient(fileName);
            if (fileClient.exists()) {
                fileClient.delete();
            }

            // 5. Fetch Document Metadata
            if (appianDocumentId == null) {
                throw new IllegalArgumentException("Appian Document is null or missing.");
            }

            Document doc = (Document) contentService.getVersion(appianDocumentId, ContentConstants.VERSION_CURRENT);
            long documentSize = doc.getSize() != null ? doc.getSize().longValue() : 0L;

            // 6. Manual Chunked Streaming to bypass Azure's 4MB limit
            inputStream = contentService.getDocumentInputStream(appianDocumentId);

            // Allocate the file size in Azure
            fileClient.create(documentSize);

            // Stream the file in 4MB chunks
            try (OutputStream azureOutStream = fileClient.getFileOutputStream()) {
                byte[] buffer = new byte[4 * 1024 * 1024]; // 4MB buffer
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    azureOutStream.write(buffer, 0, bytesRead);
                }
            }

            isSuccess = true;
            errorMessage = null;

        } catch (Exception e) {
            LOG.error(String.format("Error uploading file into Azure. Doc content id is %s and internal file name is %s",
                    appianDocumentId, fileName), e);

            isSuccess = false;
            errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ex) {
                    LOG.error("Failed to close document input stream", ex);
                }
            }
        }
    }

    // --- Inputs ---
    @Input(required = Required.ALWAYS)
    @Name("AccountUrl")
    public void setAccountUrl(String accountUrl) { this.accountUrl = accountUrl; }

    @Input(required = Required.ALWAYS)
    @Name("ShareName")
    public void setShareName(String shareName) { this.shareName = shareName; }

    @Input(required = Required.OPTIONAL)
    @Name("DirectoryPath")
    public void setDirectoryPath(String directoryPath) { this.directoryPath = directoryPath; }

    @Input(required = Required.ALWAYS)
    @Name("FileName")
    public void setFileName(String fileName) { this.fileName = fileName; }

    @Input(required = Required.ALWAYS)
    @Name("AppianDocumentId")
    public void setAppianDocumentId(Long appianDocumentId) { this.appianDocumentId = appianDocumentId; }

    @Input(required = Required.OPTIONAL)
    @Name("ManualSasToken")
    public void setManualSasToken(String manualSasToken) { this.manualSasToken = manualSasToken; }

    @Input(required = Required.OPTIONAL)
    @Name("ScsKey")
    public void setScsKey(String scsKey) { this.scsKey = scsKey; }

    @Input(required = Required.OPTIONAL)
    @Name("ScsField")
    public void setScsField(String scsField) { this.scsField = scsField; }

    // --- Outputs ---
    @Name("IsSuccess")
    public Boolean getIsSuccess() { return isSuccess; }

    @Name("ErrorMessage")
    public String getErrorMessage() { return errorMessage; }
}